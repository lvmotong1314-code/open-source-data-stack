with trips as (

    select *
    from {{ ref('stg_taxi_trips') }}

),

final as (

    select
        trip_id,
        taxi_id,

        case
            when trip_start_timestamp is null then 0
            else to_char(trip_start_timestamp::date, 'YYYYMMDD')::integer
        end as start_date_key,

        case
            when trip_end_timestamp is null then 0
            else to_char(trip_end_timestamp::date, 'YYYYMMDD')::integer
        end as end_date_key,

        case
            when pickup_community_area is null
             and pickup_census_tract is null
             and pickup_centroid_latitude is null
             and pickup_centroid_longitude is null
                then md5('__UNKNOWN_LOCATION__')
            else md5(
                coalesce(pickup_community_area::text, '__NULL__')
                || '|'
                || coalesce(pickup_census_tract, '__NULL__')
                || '|'
                || coalesce(pickup_centroid_latitude::text, '__NULL__')
                || '|'
                || coalesce(pickup_centroid_longitude::text, '__NULL__')
            )
        end as pickup_location_key,

        case
            when dropoff_community_area is null
             and dropoff_census_tract is null
             and dropoff_centroid_latitude is null
             and dropoff_centroid_longitude is null
                then md5('__UNKNOWN_LOCATION__')
            else md5(
                coalesce(dropoff_community_area::text, '__NULL__')
                || '|'
                || coalesce(dropoff_census_tract, '__NULL__')
                || '|'
                || coalesce(dropoff_centroid_latitude::text, '__NULL__')
                || '|'
                || coalesce(dropoff_centroid_longitude::text, '__NULL__')
            )
        end as dropoff_location_key,

        case
            when nullif(trim(company), '') is null
                then md5('__UNKNOWN_COMPANY__')
            else md5(nullif(trim(company), ''))
        end as company_key,

        trip_start_timestamp,
        trip_end_timestamp,

        payment_type,

        trip_seconds,
        trip_miles,
        fare,
        tips,
        tolls,
        extras,
        trip_total,

        _batch_id,
        _source_name,
        _ingested_at

    from trips

)

select *
from final