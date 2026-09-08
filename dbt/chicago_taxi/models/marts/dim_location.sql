with pickup_locations as (

    select
        pickup_community_area as community_area,
        pickup_census_tract as census_tract,
        pickup_centroid_latitude as centroid_latitude,
        pickup_centroid_longitude as centroid_longitude

    from {{ ref('stg_taxi_trips') }}

),

dropoff_locations as (

    select
        dropoff_community_area as community_area,
        dropoff_census_tract as census_tract,
        dropoff_centroid_latitude as centroid_latitude,
        dropoff_centroid_longitude as centroid_longitude

    from {{ ref('stg_taxi_trips') }}

),

all_locations as (

    select * from pickup_locations

    union all

    select * from dropoff_locations

),

distinct_locations as (

    select distinct
        community_area,
        census_tract,
        centroid_latitude,
        centroid_longitude

    from all_locations

),

known_locations as (

    select
        md5(
            coalesce(community_area::text, '__NULL__')
            || '|'
            || coalesce(census_tract, '__NULL__')
            || '|'
            || coalesce(centroid_latitude::text, '__NULL__')
            || '|'
            || coalesce(centroid_longitude::text, '__NULL__')
        ) as location_key,

        community_area,
        census_tract,
        centroid_latitude,
        centroid_longitude

    from distinct_locations

    where community_area is not null
       or census_tract is not null
       or centroid_latitude is not null
       or centroid_longitude is not null

),

unknown_location as (

    select
        md5('__UNKNOWN_LOCATION__') as location_key,
        null::integer as community_area,
        null::text as census_tract,
        null::numeric as centroid_latitude,
        null::numeric as centroid_longitude

)

select *
from unknown_location

union all

select *
from known_locations